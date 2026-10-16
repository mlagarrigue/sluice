package quic

import (
	"net"
	"time"
)

// AppLimitedMarks is how many writes on c found the sender
// application-limited: what the external tests over an emulated path read.
func AppLimitedMarks(c *Conn) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appLimitedMarks
}

// NewDelayedPC is newDelayedPC for the external tests: pc with every
// datagram it writes held delay on the way, a round trip to measure.
func NewDelayedPC(pc net.PacketConn, delay time.Duration) net.PacketConn {
	return newDelayedPC(pc, delay)
}
