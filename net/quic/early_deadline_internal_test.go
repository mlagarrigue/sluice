package quic

import (
	"context"
	"testing"
	"time"
)

// DialEarly's handshake runs on a context not canceled with the caller's,
// since it outlives an early return, and that context dropped the deadline
// too: under a ctx longer than DefaultHandshakeTimeout, the handshake gave
// up at the default where Dial waits for ctx. It keeps a longer deadline
// now, and a shorter one stays DialEarly's own wait's to keep.
func TestDialEarlyKeepsALongerDeadlineForTheHandshake(t *testing.T) {
	now := time.Now()
	long, cancel := context.WithDeadline(context.Background(), now.Add(3*DefaultHandshakeTimeout))
	defer cancel()
	hs, stop := earlyHandshakeContext(long, now)
	defer stop()
	if d, ok := hs.Deadline(); !ok || !d.Equal(now.Add(3*DefaultHandshakeTimeout)) {
		t.Errorf("under a ctx of %v the handshake's deadline is %v (set %v), want ctx's", 3*DefaultHandshakeTimeout, d, ok)
	}
	cancel()
	if hs.Err() != nil {
		t.Error("canceling the caller's ctx canceled the handshake: it outlives an early return")
	}

	short, cancel2 := context.WithDeadline(context.Background(), now.Add(DefaultHandshakeTimeout/2))
	defer cancel2()
	hs2, stop2 := earlyHandshakeContext(short, now)
	defer stop2()
	if _, ok := hs2.Deadline(); ok {
		t.Error("a ctx shorter than the default gave the handshake its deadline: after an early return the default applies")
	}
}
