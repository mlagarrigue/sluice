package quic

import (
	"net"
	"testing"
	"time"
)

// RemoteAddr answers while another goroutine holds mu — a writer inside
// sendmsg, the read loop — and answers the address the last migration
// left, not the one the connection opened with.
func TestRemoteAddrDoesNotWaitForMu(t *testing.T) {
	first := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443}
	moved := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 443}
	var c Conn
	if got := c.RemoteAddr(); got != nil {
		t.Errorf("a Conn with no peer reports %v", got)
	}
	c.mu.Lock()
	c.setPeerLocked(first)
	c.setPeerLocked(moved)
	got := make(chan net.Addr, 1)
	go func() { got <- c.RemoteAddr() }()
	select {
	case a := <-got:
		if a != net.Addr(moved) {
			t.Errorf("RemoteAddr = %v after a migration to %v", a, moved)
		}
	case <-time.After(5 * time.Second):
		t.Error("RemoteAddr waited for mu")
	}
	c.mu.Unlock()
}
