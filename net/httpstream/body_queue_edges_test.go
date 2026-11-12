package httpstream

import (
	"bytes"
	"errors"
	"testing"
)

// A streamed HTTP/2 or HTTP/3 body of exactly its bound is read whole,
// whether it was buffered before it streamed or arrives after; one byte
// more is refused.
func TestABodyQueueOfTheBound(t *testing.T) {
	cfg := Config{MaxBodyBytes: 8}.mustResolve()
	q := newBodyQueue(cfg, 1, []byte("12345678"), -1, false, 0)
	if data, err := q.next(); err != nil || string(data) != "12345678" {
		t.Errorf("buffered at the bound: %q, %v", data, err)
	}
	q = newBodyQueue(cfg, 1, []byte("123456789"), -1, false, 0)
	if _, err := q.next(); !errors.Is(err, ErrTooLarge) {
		t.Errorf("buffered one byte over: %v, want ErrTooLarge", err)
	}

	q = newBodyQueue(cfg, 1, nil, -1, false, 0)
	q.add([]byte("1234"))
	q.add([]byte("5678"))
	if err := q.end(); err != nil {
		t.Fatalf("end: %v", err)
	}
	var got []byte
	for {
		data, err := q.next()
		if err != nil || data == nil {
			if err != nil || string(got) != "12345678" {
				t.Errorf("arriving at the bound: %q, %v", got, err)
			}
			break
		}
		got = append(got, data...)
	}
}

// Over HTTP/3 every byte the body drops goes back to the transport as
// credit, the held ones with those that crossed the bound, and nothing is
// given back when there is nothing to give.
func TestABodyQueueGivesBackWhatItDrops(t *testing.T) {
	var given []int
	q := newBodyQueue(Config{MaxBodyBytes: 8}.mustResolve(), 1, nil, -1, false, 0)
	q.release = func(n int) { given = append(given, n) }
	q.add([]byte("123"))
	q.add([]byte("456789"))
	if len(given) != 1 || given[0] != 9 {
		t.Errorf("credit given back past the bound: %v, want [9]", given)
	}

	given = nil
	q = newBodyQueue(Config{}.mustResolve(), 1, nil, -1, false, 0)
	q.release = func(n int) { given = append(given, n) }
	q.close()
	if len(given) != 0 {
		t.Errorf("credit given back by an empty body: %v, want none", given)
	}
}

// An HTTP/2 DATA frame may overshoot the stream's window by its grace, and
// not by a byte more.
func TestABodyQueueWindowAndItsGrace(t *testing.T) {
	cfg := Config{}.mustResolve()
	for used, ok := range map[int32]bool{15: true, 16: false} {
		q := newBodyQueue(cfg, 1, nil, -1, false, 0)
		q.window, q.grace = 10, 5
		err := q.receive(used, make([]byte, used), false)
		if ok != (err == nil) {
			t.Errorf("%d bytes against a window of 10 and a grace of 5: %v", used, err)
		}
	}
}

// The window goes back once it is owed half the initial window, rounded
// up, and not before.
func TestABodyQueueGrantsAtHalfTheWindow(t *testing.T) {
	for _, tt := range []struct {
		initial, window, want int32
	}{
		{10, 6, 0},
		{10, 5, 5},
		{11, 6, 0},
		{11, 5, 6},
	} {
		q := newBodyQueue(Config{}.mustResolve(), 1, nil, -1, false, 0)
		q.h2 = &h2Conn{}
		q.initial, q.window = tt.initial, tt.window
		if got := q.grantLocked(); got != tt.want {
			t.Errorf("initial %d, window %d: granted %d, want %d", tt.initial, tt.window, got, tt.want)
		}
	}
}

// A declared length of zero is held to like any other.
func TestABodyQueueDeclaredEmpty(t *testing.T) {
	q := newBodyQueue(Config{}.mustResolve(), 1, nil, 0, false, 0)
	q.add([]byte("abc"))
	if err := q.end(); !errors.Is(err, ErrMalformed) {
		t.Errorf("three bytes against a content-length of 0: %v, want ErrMalformed", err)
	}
}

// Bytes that wrap around the ring come out in order, in two pieces.
func TestABodyQueueAcrossTheRingsEnd(t *testing.T) {
	q := newBodyQueue(Config{MaxBodyBytes: 1 << 20}.mustResolve(), 1, nil, -1, false, 0)
	a := bytes.Repeat([]byte("a"), 12<<10)
	b := bytes.Repeat([]byte("b"), 2<<10)
	c := append(bytes.Repeat([]byte("c"), 2<<10), bytes.Repeat([]byte("d"), 4<<10)...)
	q.add(a)
	if data, err := q.next(); err != nil || !bytes.Equal(data, a) {
		t.Fatalf("first piece: %d bytes, %v", len(data), err)
	}
	q.add(b)
	if data, err := q.next(); err != nil || !bytes.Equal(data, b) {
		t.Fatalf("second piece: %d bytes, %v", len(data), err)
	}
	q.add(c)
	if len(q.buf) != bodyMinRing {
		t.Fatalf("the ring grew to %d bytes: the test does not wrap it", len(q.buf))
	}
	first, err := q.next()
	if err != nil || !bytes.Equal(first, c[:2<<10]) {
		t.Fatalf("the piece before the ring's end: %d bytes, %v", len(first), err)
	}
	second, err := q.next()
	if err != nil || !bytes.Equal(second, c[2<<10:]) {
		t.Fatalf("the piece after it: %d bytes, %v", len(second), err)
	}
}
