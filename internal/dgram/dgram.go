// Package dgram is a pair of packet connections wired to each other in
// memory. It exists for tests, and for one specific failure they had. Only
// test files import it, so it never reaches a user's binary.
//
// A test that runs the QUIC connection over loopback UDP inherits the
// kernel's right to drop a datagram: on an idle machine that never happens
// and the test passes, and under a loaded `-race` suite it happens often
// enough to fail now and then — recovered by retransmission since RFC 9002
// loss recovery landed, but on the recovery's own clock, which turns a
// timing-sensitive assertion into a flaky one with a message that says
// nothing about the thing being asserted.
//
// A flaky test is worse than a missing one. It trains everybody to re-run
// until green, and the day it fails for a real reason nobody looks.
//
// What this keeps is everything those tests are for: a real TLS 1.3 handshake,
// real packet protection, two endpoints that share no memory beyond the bytes
// they send each other. What it removes is the transport's right to lose them.
// Datagrams arrive whole, in order, exactly once.
//
// It is deliberately not a network simulator. Loss and reordering are what a
// QUIC implementation must be tested against, and testing them belongs with
// the loss recovery that answers them — not smuggled into a handshake test
// that would then fail for two unrelated reasons at once.
//
// One divergence from a real [net.PacketConn] is accepted: deadlines are
// sampled when ReadFrom or WriteTo is entered. A SetReadDeadline issued while
// a read is already blocked does not wake it, where a real socket's would.
// The tests this package serves set their deadlines before they block, so
// the shortcut has no victim — but a test that wants to interrupt a blocked
// read must Close the pair instead.
package dgram

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// queue is how many datagrams one end may hold before a sender waits. It is
// generous rather than tuned: the point is that a test never loses a packet,
// and a bound is still stated because an unbounded queue is a test that hides
// a runaway producer instead of failing on it.
const queue = 1024

// Pair returns two packet connections, each addressed as the other's peer.
//
// Close either one and both report [net.ErrClosed] to whoever is blocked on
// them; the pair is one object with two ends.
func Pair() (left, right net.PacketConn) {
	a := &conn{local: addr("dgram:a"), in: make(chan packet, queue), closed: make(chan struct{})}
	b := &conn{local: addr("dgram:b"), in: make(chan packet, queue), closed: make(chan struct{})}
	a.peer, b.peer = b, a
	return a, b
}

type addr string

func (a addr) Network() string { return "dgram" }
func (a addr) String() string  { return string(a) }

type packet struct {
	from addr
	data []byte
}

type conn struct {
	local addr
	peer  *conn
	in    chan packet

	mu  sync.Mutex
	rdl time.Time
	wdl time.Time

	once   sync.Once
	closed chan struct{}
}

// ReadFrom returns one whole datagram, blocking until one arrives, the
// deadline passes, or the pair is closed.
//
// A datagram too large for p is truncated rather than split, which is what a
// real one does and what a caller sizing its buffer from the maximum packet
// size is relying on.
func (c *conn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.mu.Lock()
	dl := c.rdl
	c.mu.Unlock()
	// Closure is checked before the select rather than inside it. A select
	// with several ready cases picks one at random, so an end that is already
	// closed would still hand back a queued datagram now and then — which is
	// a harness that is flaky about being closed, the exact defect this
	// package exists to remove.
	if c.isClosed() {
		return 0, nil, net.ErrClosed
	}
	// A deadline already past is checked the same way and for the same
	// reason: a real socket fails the call before looking for data, while
	// the select below, with an expired timer and a queued datagram both
	// ready, would pick one at random.
	if expired(dl) {
		return 0, nil, os.ErrDeadlineExceeded
	}
	var timeout <-chan time.Time
	if !dl.IsZero() {
		t := time.NewTimer(time.Until(dl))
		defer t.Stop()
		timeout = t.C
	}
	select {
	case pkt := <-c.in:
		return copy(p, pkt.data), pkt.from, nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	case <-c.peer.closed:
		return 0, nil, net.ErrClosed
	case <-timeout:
		return 0, nil, os.ErrDeadlineExceeded
	}
}

// expired reports a deadline that is set and already past.
func expired(dl time.Time) bool {
	return !dl.IsZero() && !time.Now().Before(dl)
}

// isClosed reports whether this end or the other one has been closed.
func (c *conn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	case <-c.peer.closed:
		return true
	default:
		return false
	}
}

// WriteTo delivers one datagram to the other end.
//
// The payload is copied, because a caller reuses its buffer the moment this
// returns — the same contract a real socket has, and the one a test that
// borrowed instead would pass under until it did not.
func (c *conn) WriteTo(p []byte, a net.Addr) (int, error) {
	c.mu.Lock()
	dl := c.wdl
	c.mu.Unlock()
	if a == nil || a.String() != c.peer.local.String() {
		// There is exactly one other end. Writing anywhere else is a mistake
		// in the test rather than a packet to drop silently.
		return 0, fmt.Errorf("dgram: %v is not the other end of this pair", a)
	}
	if c.isClosed() {
		return 0, net.ErrClosed
	}
	if expired(dl) {
		return 0, os.ErrDeadlineExceeded // as ReadFrom: decided before the select
	}
	pkt := packet{from: c.local, data: append([]byte(nil), p...)}

	var timeout <-chan time.Time
	if !dl.IsZero() {
		t := time.NewTimer(time.Until(dl))
		defer t.Stop()
		timeout = t.C
	}
	select {
	case c.peer.in <- pkt:
		return len(p), nil
	case <-c.closed:
		return 0, net.ErrClosed
	case <-c.peer.closed:
		return 0, net.ErrClosed
	case <-timeout:
		return 0, os.ErrDeadlineExceeded
	}
}

func (c *conn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *conn) LocalAddr() net.Addr { return c.local }

func (c *conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rdl, c.wdl = t, t
	return nil
}

func (c *conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rdl = t
	return nil
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wdl = t
	return nil
}
