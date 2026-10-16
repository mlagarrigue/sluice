package quic

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// A frame on the last stream the limit allows opens every stream of its kind
// at once (RFC 9000 §2.1), a hundred under the defaults. Queuing them for
// AcceptStream must not block the read loop: the 64-deep channel that used
// to stand there stopped it on the 65th, with nobody accepting.
func TestTheLastStreamOfTheLimitDoesNotBlockReception(t *testing.T) {
	params := DefaultParameters()
	c := newConn(&fuzzSink{addr: "accept:self"}, fakeFuzzAddr("accept:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, params)
	defer c.Close()

	last := 4 * (params.InitialMaxStreamsBidi - 1)
	done := make(chan error, 1)
	go func() { done <- c.frames(spaceApplication, streamFrame(nil, last, 0, "x", true), time.Now()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("the frame on stream %d still not processed after 2 s", last)
	}
	for want := uint64(0); want <= last; want += 4 {
		s, err := c.AcceptStream(t.Context())
		if err != nil || s.id != want {
			t.Fatalf("AcceptStream = %v, %v; want stream %d", s, err, want)
		}
	}
}

// A stream the application picked by identifier before the peer opened it
// is the one the peer's frames reach and AcceptStream hands out: the loop
// that opens the peer's streams used to replace it with a fresh one.
func TestAStreamPickedBeforeThePeerOpensItIsKept(t *testing.T) {
	c := newConn(&fuzzSink{addr: "accept:self"}, fakeFuzzAddr("accept:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	defer c.Close()

	picked, err := c.Stream(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.frames(spaceApplication, streamFrame(nil, 4, 0, "req", true), time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for _, want := range []uint64{0, 4} {
		s, err := c.AcceptStream(ctx)
		if err != nil || s.id != want {
			t.Fatalf("AcceptStream = %v, %v; want stream %d", s, err, want)
		}
		if want == 4 && s != picked {
			t.Fatal("AcceptStream handed out a stream other than the one Conn.Stream returned")
		}
	}
	buf := make([]byte, 8)
	if n, _ := picked.Read(buf); string(buf[:n]) != "req" {
		t.Fatalf("the picked stream read %q, want \"req\"", buf[:n])
	}
}

// A context already done that finds no credit still tells the peer it is
// blocked (RFC 9000 §4.6, §19.14): STREAMS_BLOCKED of the kind asked for,
// carrying the limit, before the context's error comes back.
func TestATryOpenWithoutCreditSendsStreamsBlocked(t *testing.T) {
	for _, uni := range []bool{false, true} {
		c := newConn(&fuzzSink{addr: "open:self"}, fakeFuzzAddr("open:peer"),
			[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, true, DefaultParameters())
		c.hasPeerParams = true // limits of zero: no credit of either kind
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := c.open(ctx, uni); !errors.Is(err, context.Canceled) {
			t.Fatalf("uni=%v: open = %v; want context.Canceled", uni, err)
		}
		want := appendFrame(nil, frameStreamsBlocked, 0)
		if uni {
			want = appendFrame(nil, frameStreamsBlocked|1, 0)
		}
		if !bytes.Equal(c.control, want) {
			t.Errorf("uni=%v: control queue %x; want STREAMS_BLOCKED %x", uni, c.control, want)
		}
		c.Close()
	}
}
