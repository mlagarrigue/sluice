package quic

// AppLimitedMarks is how many writes on c found the sender
// application-limited: what the external tests over an emulated path read.
func AppLimitedMarks(c *Conn) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appLimitedMarks
}
