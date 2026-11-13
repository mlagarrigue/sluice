package quic

import (
	"context"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// A peer challenges from a second address; the server's PATH_RESPONSE is
// the only packet in flight, recorded off the path in use, and drain — a
// Shutdown waiting for the flight to empty — is parked on cond. The peer
// then acknowledges that packet alone. The flight is empty, and drain must
// return: the ACK released no byte of the path in use, so the window wake
// skipped it, and drain stayed parked until its context or the idle timeout
// ended it.
func TestAnAckOfOffPathPacketsWakesDrain(t *testing.T) {
	c, _ := newMigratableServer(t, "offpath-drain")
	c.mu.Lock()
	c.amplActive = false
	c.mu.Unlock()
	if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), make([]byte, 16)), time.Now()); err != nil {
		t.Fatal(err)
	}
	ackInFlight(t, c) // nothing on the path in use is left in flight

	rxFromNewAddress(c, fakeFuzzAddr("offpath-drain:peerB"), 1200, false)
	challenge := append(varint.Append(nil, framePathChallenge), 9, 8, 7, 6, 5, 4, 3, 2)
	if err := c.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}
	rxFromNewAddress(c, fakeFuzzAddr("offpath-drain:peerA"), 60, true)

	c.mu.Lock()
	var onPath, offPath int
	for _, p := range c.spaces[spaceApplication].sent.packets {
		if p.offPath {
			offPath++
		} else {
			onPath++
		}
	}
	c.mu.Unlock()
	if offPath == 0 || onPath != 0 {
		t.Fatalf("setup: %d off-path and %d on-path packets in flight; want off-path ones only", offPath, onPath)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.drain(ctx) }()
	time.Sleep(200 * time.Millisecond) // drain reaches its wait
	select {
	case err := <-done:
		t.Fatalf("drain returned %v before the ACK", err)
	default:
	}

	ackInFlight(t, c)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain returned %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the flight emptied by an ACK of off-path packets, and drain stayed parked")
	}
}
